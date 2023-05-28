import MenuOutlinedIcon from "@mui/icons-material/MenuOutlined";
import PropTypes from "prop-types";
import { Sidebar, Menu, MenuItem, SubMenu } from "react-pro-sidebar";
import { useState } from "react";

export default function Sidenav({ data, onItemSelect }) {
  const [isCollapsed, setIsCollapsed] = useState(false);

  return (
    <div style={{ display: "flex", height: "100vh" }}>
      <Sidebar className="app" collapsed={isCollapsed}>
        <Menu>
          <MenuItem
            className="menu1"
            onClick={() => {
              setIsCollapsed(!isCollapsed);
              console.log(isCollapsed);
            }}
          >
            <h5>TRADE WITH SCIENCE</h5>
          </MenuItem>
          {Object.keys(data).map((key) => (
            <SubMenu key={key} label={key}>
              {data[key].map((item) => {
                return (
                  <MenuItem key={item} onClick={() => onItemSelect(item)}>
                    {item}
                  </MenuItem>
                );
              })}
            </SubMenu>
          ))}
        </Menu>
      </Sidebar>
    </div>
  );
}

Sidenav.propTypes = {
  data: PropTypes.objectOf(PropTypes.arrayOf(PropTypes.string)).isRequired,
  onItemSelect: PropTypes.func.isRequired,
};
